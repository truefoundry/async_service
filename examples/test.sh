#!/bin/bash

module_name="$1"

if [ -z "$module_name" ]; then
    echo "Usage: $0 <module_name>"
    echo "Example: $0 [amqp]"
    exit 1
fi

if [ -f "${module_name}" ]; then
    module_name="${module_name}"
else
    module_name="$module_name.py"
fi

python $module_name
