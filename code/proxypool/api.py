from flask import Flask, request, jsonify
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from db import * 

app = Flask("proxy_pool")
limiter = Limiter(
    app,
    key_func=get_remote_address,
    default_limits=["1440000 per day", "60000 per hour"]
)
@app.route("/slow/get_proxy", methods=["GET", "POST"])
@limiter.limit("10 per minute")
def get_proxy():
    keys = ["IP", "ProxyType", "Status", "ResponseTime", "Anonymity", "Country", "ShortCode", "Google", "ISP"]
    args = request.args
    num_proxy = args["Num"]
    proxy_type = args["ProxyType"]
    con = sqlite3.connect("data/proxy.db")
    cur = con.cursor()
    tmp_proxy_list, proxy_list = random_get_db_data(cursor = cur, proxy_type=proxy_type, num = num_proxy), []
    for i in range(0, len(tmp_proxy_list)):
        tmp_proxy_list_i = list(tmp_proxy_list[i][1:])
        d = {}
        for j in range(0, len(tmp_proxy_list_i)):
            d[keys[j]] = tmp_proxy_list_i[j]
        proxy_list.append(d)
    
    return jsonify({"data":proxy_list})

@app.route("/info", methods=["GET", "POST"])
@limiter.limit("10 per minute")
def info():
    keys = ["IP", "ProxyType", "Status", "ResponseTime", "Anonymity", "Country", "ShortCode", "Google", "ISP"]
    
    return jsonify({"return info":keys, "parameters":{"ProxyType":"http-https, https, http, socks5", "Num":"1-100"}})

if __name__ == "__main__":
    app.run()

