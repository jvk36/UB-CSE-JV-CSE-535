from flask import Flask

app = Flask(__name__)

@app.route('/')
def hello():
    return "Hello, World!"

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)


#
# To run this app in GCP do the following:
# *** Open Port 5000 using a firewall in GCP - flask uses 5000 port by default
#   export FLASK_APP=hello
#   export FLASK_ENV=development  # might be optional
#   flask run --host 0.0.0.0      # this is a special address that tells flask to listen on all IP addresses.
# Or:
#  python3 hello.py
# To access, use:
#    http://localhost:5000 or http://127.0.0.1:5000 or http://34.162.96.100:5000
#
