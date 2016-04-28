FROM node:6
MAINTAINER Octoblu <docker@octoblu.com>

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY . /usr/src/app
RUN npm install --production

CMD [ "node", "command-dispatch.js" ]
