// Copyright 2015, EMC, Inc.

'use strict';

module.exports = {
    friendlyName: 'Poller Service',
    injectableName: 'Graph.Service.Poller',
    serviceGraph: true,
    options: {
        'poller-cache': {
            schedulerOverrides: {
                timeout: -1
            }
        }
    },
    tasks: [
        {
            label: 'poller-cache',
            taskDefinition: {
                friendlyName: 'Poller cache',
                injectableName: 'Task.Inline.Poller.Cache',
                implementsTask: 'Task.Base.Message.Cache',
                options: {},
                properties: {}
            }
        }
    ]
};
