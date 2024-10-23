# syntax=docker/dockerfile:1

FROM node:18.16.1-alpine
SHELL [ "/bin/sh", "-cex" ]

# Create app directory
WORKDIR /usr/src/app

# Bundle app source
COPY . .

# Install node_modules
RUN \
  <<EOF
npm install
npm install pm2 -g
EOF

ENTRYPOINT [ "/usr/src/app/entrypoint.sh" ]
