# Backend Dockerfile (Node/Express + Postgres client)
# Build with: docker build -t duisburg-backend .
# Run with envs: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, PORT=3001 (or your choice)

FROM node:18-alpine
WORKDIR /app

COPY package*.json ./
RUN npm install --production

COPY server.js db.js ./

# Default port; override with PORT env
ENV PORT=3001

EXPOSE 3001
CMD ["node", "server.js"]
