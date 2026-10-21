FROM denoland/deno:alpine
WORKDIR /app
COPY . .
EXPOSE 8080
CMD ["deno", "run", "--allow-net", "--allow-env", "--allow-read", "call.ts"]
