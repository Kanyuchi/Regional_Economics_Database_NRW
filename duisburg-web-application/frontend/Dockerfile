# Frontend Dockerfile: build React app and serve with Nginx
# Build args:
#   VITE_API_BASE (default http://backend:3001 for docker-compose)

FROM node:18-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
ARG VITE_API_BASE=http://backend:3001
ENV VITE_API_BASE=${VITE_API_BASE}
RUN npm run build

FROM nginx:stable-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=builder /app/dist /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
