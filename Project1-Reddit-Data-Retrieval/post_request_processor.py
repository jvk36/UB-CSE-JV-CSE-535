import requests

url = "http://35.225.120.115:8000/grade_index"
data = {
"ip": "34.162.150.201",
"port": "8983",
"core": "IRF22P1",
"ubit_name": "annkonna"
}

r = requests.post(url, json=data)
print(r)
