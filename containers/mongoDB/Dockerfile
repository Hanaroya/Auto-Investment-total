FROM mongo:latest

COPY init-mongo.js /docker-entrypoint-initdb.d/
COPY mongod.conf /etc/mongod.conf

EXPOSE 27017

CMD ["mongod", "--config", "/etc/mongod.conf"] 