#!/usr/bin/env bash
if [ -f search_sync.pid ]; then
    kill `cat search_sync.pid`
fi
