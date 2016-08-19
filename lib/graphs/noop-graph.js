// Copyright 2015, EMC, Inc.

'use strict';

module.exports = {
    friendlyName: 'noop-graph',
    injectableName: 'Graph.noop-example',
    options: {
        'noop-2': {
            delay: 30
        }
    },
    tasks: [
        {
            label: 'noop-1',
            taskName: 'Task.noop'
        },
        {
            label: 'noop-2',
            taskName: 'Task.noop',
            waitOn: {
                'noop-1': 'finished'
            }
        },
        {
            label: 'parallel-noop-1',
            taskName: 'Task.noop',
            waitOn: {
                'noop-2': 'finished'
            }
        },
        {
            label: 'parallel-noop-2',
            taskName: 'Task.noop',
            waitOn: {
                'noop-2': 'finished'
            }
        },
    ]
};
