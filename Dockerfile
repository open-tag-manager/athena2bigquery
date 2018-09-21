FROM node:6.11.5-alpine

WORKDIR /usr/src/app

RUN npm install -g yarn

COPY package.json .
COPY yarn.lock .
COPY app.js .

RUN yarn install

CMD ["node", "app.js"]
