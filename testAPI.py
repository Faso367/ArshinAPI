import requests

#url = 'http://localhost:5000/hello'
url = 'http://localhost:5000/vri'
headers = {
    'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyIjpudWxsLCJleHAiOjE3MjE5OTYxNDd9.ao4jhe87wHxRDu25EcqT7sZ1eQNyRIJ1lTlGxXLgrJU'
}
params = {
    'svidetelstvoNumber' : 'С-ДВК/02-05-2024/338211837',
    'rows' : 101,
    'start' : 1,
    'isPrigodno' : 'true'
    #'password': 'Hello'
}

response = requests.get(url, headers=headers, params=params)

print(response.json())
