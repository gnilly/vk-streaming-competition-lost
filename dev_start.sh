#! /bin/bash

#brunch w -s -d &
PORT=5000 ENV=DEVELOPMENT DATABASE_URL='postgres://localhost/emojidb' gunicorn web:main --config config/gunicorn.conf --reload

