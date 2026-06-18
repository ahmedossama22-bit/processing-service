FROM node:18-alpine
WORKDIR /usr/src/app/processing-service
COPY package*.json ./
RUN npm install
COPY . .
EXPOSE 3002
CMD ["npm", "start"]
