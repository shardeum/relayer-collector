# syntax=docker/dockerfile:1

## global args
ARG NODE_VERSION=18.16.1

FROM node:${NODE_VERSION}
SHELL [ "/bin/bash", "-cex" ]

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