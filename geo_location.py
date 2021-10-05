from flask import Flask
from flask import Response

app = Flask(__name__)


@app.route('/')
@app.route('/hello')
def HelloWorld():
    return Response(status=200)


if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0', port=8080)
