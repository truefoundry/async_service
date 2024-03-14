#!/bin/bash

module_name="$1"

if [ -z "$module_name" ]; then
    echo "Usage: $0 <module_name>"
    echo "Example: $0 [amqp]"
    exit 1
fi

if [ -f "${module_name}" ]; then
    module_name="${module_name%.py}"
else
    module_name="$module_name"
fi

gunicorn "$module_name:app" --workers 1 --worker-class uvicorn.workers.UvicornWorker --bind 127.0.0.1:8000
