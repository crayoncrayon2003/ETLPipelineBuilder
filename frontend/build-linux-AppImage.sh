set -e

if [ ! -d "node_modules" ]; then
    npm install
fi

npm run build-linux-AppImage