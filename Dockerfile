FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY src ./src

# Back4App: set Port = 3000 in App Settings
EXPOSE 3000

# Run node directly (npm start can hide/buffer logs in containers)
CMD ["node", "src/index.js"]
