docker run -p 5000:5000 -v `cd ../ && pwd`:/app -w /app/solution  -it --rm nyurik/alpine-python3-requests  /bin/sh -c "pip3 install -Ur requirements.txt; python3 app.py"
