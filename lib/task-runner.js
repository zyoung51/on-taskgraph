// Copyright 2016, EMC, Inc.

'use strict';

var di = require('di');
var Ringpop = require('ringpop');
var TChannel = require('tchannel');
var TChannelJSON = require('tchannel/as/json');

module.exports = taskRunnerFactory;
di.annotate(taskRunnerFactory, new di.Provide('TaskGraph.TaskRunner'));
di.annotate(taskRunnerFactory,
    new di.Inject(
        'Logger',
        'Promise',
        'Constants',
        'Assert',
        'uuid',
        '_',
        'Rx',
        'Task.Task',
        'TaskGraph.Store'
    )
);

function taskRunnerFactory(
    Logger,
    Promise,
    Constants,
    assert,
    uuid,
    _,
    Rx,
    Task,
    store
) {
    var logger = Logger.initialize(taskRunnerFactory);

    /**
     * The taskRunner runs tasks which are sent over AMQP by a scheduler; a runner
     * will only run tasks that share its domain
     *
     * @param {Object} options
     * @param {String} options.domain - The scheduling domain to accept tasks from
     * @param {Object} options.heartbeatInterval - Rx.js observable interval to schedule heartbeats
     * @param {Number} options.lostBeatLimit - The number of heartbeats to allow
     * an inactive task before expiring its lease
     * @constructor
     */
    function TaskRunner(options) {
        options = options || {};
        this.lostTasks = {};
        this.taskRunnerId = uuid.v4();
        this.completedTasks = [];
        this.runTaskStream = new Rx.Subject();
        this.cancelTaskStream = new Rx.Subject();
        this.heartbeat = Rx.Observable.interval(options.heartbeatInterval || 1000);
        this.subscriptions = [];
        this.running = false;
        this.activeTasks = {};
        this.lostBeatLimit = options.lostBeatLimit || 3;
        this.domain = options.domain || Constants.Task.DefaultDomain;
        this.tchannel = new TChannel();
        this.ringpop = new Ringpop({
            app: 'on-taskgraph',
            hostPort: '127.0.0.1:3000',  // ultimately ignored
            channel: this.tchannel.makeSubChannel({
                serviceName: 'ringpop',
                trace: false
            })
        });
    }

    /**
     * Returns true if the TaskRunner is running, false otherwise
     * @returns {Boolean}
     * @memberOf TaskRunner
     */
    TaskRunner.prototype.isRunning = function() {
        return this.running;
    };


    /**
     * Initializes all permanent observable pipelines to handle
     * running, heartbeating, and cancelling tasks
     *
     * @memberOf TaskRunner
     */
    TaskRunner.prototype.initializePipeline = function() {
        this.createRunTaskSubscription(this.runTaskStream).subscribe(
            this.handleStreamSuccess.bind(this, 'Task finished'),
            this.handleStreamError.bind(this, 'Task failure')
        );
        this.createHeartbeatSubscription(this.heartbeat).subscribe(
                this.handleStreamSuccess.bind(this, null),
                this.handleStreamError.bind(this, 'Error handling heartbeat failure')
        );
        this.createCancelTaskSubscription(this.cancelTaskStream)
        .subscribe(
            this.handleStreamSuccess.bind(this, 'Task cancelled'),
            this.handleStreamError.bind(this, 'Task cancellation error')
        );
    };

    /**
     * Wraps promise or observable returning functions in their own new observable
     * sequence to catch failures
     *
     * @memberOf TaskRunner
     * @param {Object} toObserve - Promise/Observable to wrap
     * @param {String} msg - an error message for if the wrapped observable fails
     * @param {Object} the data to be mapped with the given Promise/Observable
     * @returns {Observable}
     */
    TaskRunner.prototype.safeStream = function(toObserve, msg, streamData) {
        var self = this;
        return Rx.Observable.just(streamData)
            .flatMap(toObserve)
            .catch(self.handleStreamError.bind(self,
                        msg || 'An unhandled Error occured in the safe task stream'));
    };

    /**
     * Creates the Rx.Observable pipeline for running tasks:
     * Checks out a task, then gets the task defition, then instantiates and runs the task
     *
     * @param {Object} runTaskStream
     * @returns {Observable}
     * @memberOf TaskRunner
     */
    TaskRunner.prototype.createRunTaskSubscription = function(runTaskStream) {
        var self = this;
        return runTaskStream
            .takeWhile(self.isRunning.bind(self))
            .filter(function(taskData) {
                return !_.has(self.activeTasks, taskData.taskId);
            })
            .flatMap(self.safeStream.bind(
                        self,
                        store.checkoutTask.bind(store, self.taskRunnerId),
                        'Error checking out task'))
            .filter(function(data) { return !_.isEmpty(data);})
            .flatMap(self.safeStream.bind(self, store.getTaskById, 'Error fetching task data'))
            .flatMap(self.runTask.bind(self));
    };

    /**
     * Creates the Rx.Observable pipeline for cancelling tasks
     *
     * @param {Object} cancelTaskStream
     * @returns {Observable}
     * @memberOf TaskRunner
     */
    TaskRunner.prototype.createCancelTaskSubscription = function(cancelTaskStream) {
        var self = this;
        return cancelTaskStream
            .takeWhile(self.isRunning.bind(self))
            .flatMap(self.cancelTask.bind(self));
    };

    /**
     * Cancel a task
     *
     * @param {Object} data
     * @returns {Observable}
     * @memberOf TaskRunner
     */
    TaskRunner.prototype.cancelTask = function(data) {
        var self = this;
        return Rx.Observable.just(data)
            .map(function(taskData) {
                return self.activeTasks[taskData.taskId];
            })
            .filter(function(task) { return !_.isEmpty(task); })
            .tap(function(task) {
                logger.info('Cancelling task', { taskId: task.instanceId });
            })
            .flatMap(function(task) { return task.cancel(); })
            .map(function() { return { taskId: data.taskId }; })
            .finally(function() {
                delete self.activeTasks[data.taskId];
            });
    };

    /**
     * Creates the heartbeat pipeline from the given observable
     *
     * @param {Object} heartInterval
     * @returns {Observable}
     * @memberOf TaskRunner
     */
    TaskRunner.prototype.createHeartbeatSubscription = function(heartInterval) {
        var self = this;
        return  heartInterval
                .takeWhile(self.isRunning.bind(self))
                .flatMap(store.heartbeatTasksForRunner.bind(store, self.taskRunnerId))
                .flatMap( function(taskCount) {
                    if(taskCount < Object.keys(self.activeTasks).length){
                        return self.handleUnownedTasks();
                    } else if (taskCount > Object.keys(self.activeTasks).length) {
                        return self.handleLostTasks();
                    }
                    return Rx.Observable.just(null);
                })
                .catch(function(error) {
                    logger.error('Failed to update heartbeat, stopping task runner and tasks', {
                        taskRunnerId: self.taskRunnerId,
                        error: error,
                        activeTasks: _.keys(self.activeTasks)
                    });
                    return Rx.Observable.just(self.stop.bind(self)());
                });
    };

    /**
     * Records the number of times specific tasks have been heartbeated while not active
     * expires the leases on tasks which exceed the limit for heartbeats without being
     * added to active tasks
     *
     * @returns {Observable}
     * @memberOf TaskRunner
     */
    TaskRunner.prototype.handleLostTasks = function() {
        var self = this;
        return Rx.Observable.fromPromise(store.getOwnTasks(self.taskRunnerId))
            .flatMap(function(ownTasks) {
                _.difference(_.pluck(ownTasks, 'taskId'), _.keys(self.activeTasks))
                    .forEach(function(taskId){
                        if(!self.lostTasks[taskId]) {
                            self.lostTasks[taskId] = 0;
                        }
                            self.lostTasks[taskId] += 1;
                        if(self.lostTasks[taskId] >= self.lostBeatLimit) {
                            store.expireLease(taskId);
                            delete self.lostTasks[taskId];
                        }
                    });
                return Rx.Observable.just(null);
            });
    };

    /**
     * Stops any tasks which are in activeTasks but are not being heartbeated
     *
     * @returns {Observable}
     * @memberOf TaskRunner
     */
    TaskRunner.prototype.handleUnownedTasks = function() {
        var self = this;
        return Rx.Observable.fromPromise(store.getOwnTasks(self.taskRunnerId))
            .flatMap(function(ownTasks) {
                _.difference(Object.keys(self.activeTasks),
                        _.pluck(ownTasks, 'taskId'))
                .forEach(function(taskId) {
                        logger.info('stopping unowned task ', {data: taskId});
                        if(self.activeTasks[taskId]) {
                            self.activeTasks[taskId].stop();
                        }
                            delete self.activeTasks[taskId];
                });
                return Rx.Observable.just(null);
            });
    };

    /**
     * Subscription function, called when any item is successfully processed by
     * the consumed Observable pipeline
     *
     * @param {String} msg
     * @param {Object} data
     * @returns {Observable}
     * @memberOf TaskRunner
     */
    TaskRunner.prototype.handleStreamSuccess = function(msg, data) {
        if (msg) {
            if (data && !data.taskRunnerId) {
                data.taskRunnerId = this.taskRunnerId;
            }
            logger.info(msg, data);
        }
        return Rx.Observable.empty();
    };

    /**
     * Subscription function, called when an error is emitted on the consumed
     * Observable pipeline
     *
     * @param {String} msg
     * @param {Error} err
     * @returns {Observable}
     * @memberOf TaskRunner
     */
    TaskRunner.prototype.handleStreamError = function(msg, err) {
        logger.error(msg, {
            taskRunnerId: this.taskRunnerId,
            // stacks on some error objects don't get printed if part of
            // the error object so separate them out here
            error: _.omit(err, 'stack'),
            stack: err.stack
        });
        return Rx.Observable.empty();
    };

    /**
     * Instantiates, runs, and publishes the status of a task
     *
     * @param {Object} data
     * @returns {Observable}
     * @memberOf TaskRunner
     */
    TaskRunner.prototype.runTask = function(data) {
        var self = this;
        return Rx.Observable.just(data)
            .flatMap(function(_data) {
                return Task.create(
                    _data.task,
                    { instanceId: _data.task.instanceId },
                    _data.context
                );
            })
            .tap(function(task) {
                self.activeTasks[task.instanceId] = task;
                logger.info("Running task ", {
                    taskRunnerId: self.taskRunnerId,
                    taskId: task.instanceId,
                    taskName: task.definition.injectableName
                });
            })
            .flatMap(function(task) {
                return task.run();
            })
            .takeWhile(function(task) { return !_.isEmpty(task);})
            .flatMap(function(task) {
                return Rx.Observable.forkJoin([
                    Rx.Observable.just(task),
                    store.setTaskState({
                        taskId: task.instanceId,
                        graphId: task.context.graphId,
                        state: task.state,
                        error: task.error,
                        context: task.context
                    })
                ]);
            })
            .map(_.first)
            .tap(function(task) {
                delete self.activeTasks[task.instanceId];
            })
            .tap(self.publishTaskFinished.bind(self))
            .map(function(task) { return _.pick(task, ['instanceId', 'state']); })
            .catch(self.handleStreamError.bind(self, 'error while running task'));
    };

    /**
     * Publishes a task finished event over AMQP
     *
     * @param {Object} task
     * @returns {Promise}
     * @memberOf TaskRunner
     */
    TaskRunner.prototype.publishTaskFinished = function(task) {
        var self = this;
        var data = {
            taskId: task.instanceId,
            graphId: task.context.graphId,
            state: task.state,
            context: task.context,
            terminalOnStates: task.definition.terminalOnStates
        };

        if (task.error && task.error.stack) {
            data.error = task.error.stack;
        } else if (task.error) {
            data.error = task.error.toString();
        }

        var hosts = _.keys(self.rpcObj.schedulers);
        return new Promise(function(resolve, reject) {
            self.rpcObj.schedulerChannel.waitForIdentified({ host: hosts[0] }, function(err) {
                var tchannelJSON = TChannelJSON();
                if(err) {
                    return reject(err);
                }

                tchannelJSON.send(self.rpcObj.schedulerChannel.request({
                    headers: {
                        cn: 'publishTaskFinished'
                    },
                    serviceName: 'scheduler',
                    hasNoParent: true,
                    timeout: 5000
                }), 'finishedTask', { domain: self.domain }, data, function(err, resp) {
                    if(!err && resp.ok) {
                        return resolve(resp);
                    }
                    reject(err);
                });
            });
        })
        .catch(function(error) {
            logger.error('Error publishing task finished event', {
                taskId: task.instanceId,
                graphId: task.context.graphId,
                state: task.state,
                error: error
            });
        });
    };

    /**
     * Stops the TaskRunner and disposes resources as necessary
     *
     * @returns {Promise}
     * @memberOf TaskRunner
     */
    TaskRunner.prototype.stop = function() {
        var self = this;
        self.running = false;
        return Promise.map(this.subscriptions, function() {
            return self.subscriptions.pop().dispose();
        });
    };

    /**
     * Starts the task runner and initializes pipelines
     *
     * @returns {Promise}
     * @memberOf TaskRunner
     */
    TaskRunner.prototype.start = function() {
        var self = this;

        return runnerRPC(self, self.ringpop, self.tchannel)
        .then(function(rpcObj) {
            self.rpcObj = rpcObj;
            self.ringpop.setupChannel();
            self.ringpop.on('ringChanged', rpcObj.onRingChanged.bind(rpcObj));
            return new Promise(function(resolve, reject) {
                self.tchannel.on('error', reject);
                self.tchannel.listen(0, '127.0.0.1', function() {
                    self.ringpop.hostPort = self.tchannel.hostPort;
                    self.ringpop.bootstrap(['127.0.0.1:3000'], function(err) {  // from config
                        if(err) {
                            reject();
                        }
                        resolve();
                    });
                });
            })
        })
        .then(function() {
            self.running = true;
            self.initializePipeline();
            logger.info('Task runner started', {
                TaskRunnerId: self.taskRunnerId,
                domain: self.domain
            });
        });
    };

    /** creates a new TaskRunner
     *
     * @param {Object} options
     * @returns {Object} TaskRunner instance
     * @memberOf TaskRunner
     */
    TaskRunner.create = function(options) {
        return new TaskRunner(options);
    };

    function runnerRPC(runner, ringpop, tchannel) {
        ringpop.appChannel = tchannel.makeSubChannel({ serviceName: 'runner' });
        ringpop.discoveryChannel = tchannel.makeSubChannel({ serviceName: 'discovery' });

        var tchannelJson = new TChannelJSON();
        tchannelJson.register(ringpop.discoveryChannel, 'info', runner, runner.rpcInfo.bind(runner) );

        tchannelJson.register(ringpop.appChannel, 'scheduleTask', runner, runner.rpcScheduleTask.bind(runner) );
        tchannelJson.register(ringpop.appChannel, 'cancelTask', runner, runner.rpcCancelTask.bind(runner) );

        return Promise.resolve({
            schedulerChannel: TChannel().makeSubChannel({ serviceName: 'scheduler' }),
            rpcChannel: tchannelJson, 
            owner: runner,
            schedulers: {},
            onRingChanged: function(event) {
                var self = this;
                _.forEach(event.added, function(peer) {
                    var client = TChannel();
                    var clientChan = client.makeSubChannel({ serviceName: 'discovery', peers: [ peer ] });
                    var tchannelJSON = TChannelJSON();
                    tchannelJSON.send(clientChan.request({
                        headers: {
                            cn: 'client'
                        },
                        serviceName: 'discovery',
                        hasNoParent: true
                    }), 'info', {}, {}, function(err, resp) {
                        if(!err && resp.ok) {
                            if( _.includes(_.get(resp, 'body.serviceName', []), 'scheduler')) {
                                console.log('found new scheduler: ' + peer );
                                self.schedulers[peer] = {
                                    uuid: resp.body.uuid,
                                    domain: resp.head.domain
                                };
                            }
                        }
                        clientChan.close();
                    });
                });
                _.forEach(event.removed, function(peer) {
                    console.log('removing scheduler: ', peer);
                    delete self.schedulers[peer];
                });
            }
        });
    }

    TaskRunner.prototype.rpcInfo = function(context, req, head, body, callback) {
        return callback(null, {
            ok: true,
            head: {
                domain: context.domain
            },
            body: { 
                serviceName: [ 'runner' ],
                uuid: context.taskRunnerId
            }
        });
    };

    TaskRunner.prototype.rpcScheduleTask = function(context, req, head, body, callback) {
        context.runTaskStream.onNext.call(context.runTaskStream, body);
        callback(null, {
            ok: true,
            head: head,
            body: { 
                status: 1
            }
        });
    };

    TaskRunner.prototype.rpcCancelTask = function(context, req, head, body, callback) {
        context.cancelTaskStream.onNext.call(context.cancelTaskStream, body);
        callback(null, {
            ok: true,
            head: head,
            body: { 
                status: 1
            }
        });
    };



    return TaskRunner;
}
