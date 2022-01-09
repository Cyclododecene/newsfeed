from flask import Flask, request, jsonify
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from db import *

app = Flask(__name__)
limiter = Limiter(
    app,
    key_func=get_remote_address,
    default_limits=["1440000 per day", "60000 per hour"]
)

@app.route("/api/v1.0/proxy/info", methods=["GET"])
@limiter.limit("10 per minute")
def info():
    return jsonify({"parameters":{"Code": "Short Code of Country or Region", "ProxyType":"http-https, https, http, socks5", "Num":"1-100"}, 
                    "return info": ["IP", "ProxyType", "Status", "ResponseTime", "Anonymity", "Country", "ShortCode", "Google", "ISP"] 
                    "example":"http://data.cklau.ac.cn/proxy/api/v1.0/getdata?ProxyType=socks5&Num=5i&Code=CN"})

@app.route("/api/v1.0/proxy/count", methods=["GET"])
@limiter.limit("10 per minute")
def count():
    con = sqlite3.connect("data/proxy.db")
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM Proxy")
    count = cur.fetchall()
    con.close()
    return jsonify({"count":count[0][0]})

@app.route("/api/v1.0/proxy/getdata", methods=["GET"])
@limiter.limit("100 per minute")
def get_proxy():
    keys = ["IP", "ProxyType", "Status", "ResponseTime", "Anonymity", "Country", "ShortCode", "Google", "ISP"]
    args = request.args
    num_proxy = args["Num"]
    proxy_type = args["ProxyType"]
    code = args["Code"]
    con = sqlite3.connect("data/proxy.db")
    cur = con.cursor()
    tmp_proxy_list, proxy_list = random_get_db_data(cursor = cur, proxy_type=proxy_type, num = num_proxy, short_code=code), []
    for i in range(0, len(tmp_proxy_list)):
        tmp_proxy_list_i = list(tmp_proxy_list[i][1:])
        d = {}
        for j in range(0, len(tmp_proxy_list_i)):
            d[keys[j]] = tmp_proxy_list_i[j]
        proxy_list.append(d)

    return jsonify({"data":proxy_list})

if __name__ == "__main__":
    app.run(host = "0.0.0.0", port = 5001)