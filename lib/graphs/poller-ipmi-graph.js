// Copyright 2016, EMC, Inc.

'use strict';

module.exports = {
    friendlyName: 'IPMI Poller Agent',
    injectableName: 'Pollers.IPMI',
    options: {
        'run-command': {
            command: null
        }
    },
    tasks: [
        {
            label: 'run-command',
            taskName: 'Task.noop' //'Task.Trigger.Command.IPMI'
        }
    ]
};
