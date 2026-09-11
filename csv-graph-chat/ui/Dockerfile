FROM nginx:1.27-alpine

# Remove default nginx config
RUN rm /etc/nginx/conf.d/default.conf

# Copy custom config
COPY nginx.conf /etc/nginx/conf.d/app.conf

# Copy static files
COPY public/ /usr/share/nginx/html/

# nginx alpine runs as nginx user by default (non-root)
EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
