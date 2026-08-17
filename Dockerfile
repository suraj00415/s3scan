# surajshah/securescope:s3scan-v1.0
# docker build -t surajshah/securescope:s3scan-v1.0 .
FROM node:22-alpine

WORKDIR /app

COPY package.json ./
RUN npm install --production

COPY s3scan.js ./

RUN addgroup -S s3scan && adduser -S s3scan -G s3scan
USER s3scan

ENTRYPOINT ["node", "s3scan.js"]
