FROM zenko/cloudserver:pensieve-0


COPY . /usr/src/app
RUN npm install
VOLUME ["/usr/src/app/localData","/usr/src/app/localMetadata"]

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]
CMD [ "npm", "start" ]

EXPOSE 8000
