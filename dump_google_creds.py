import json

with open("outcomes/google-credentials.json") as f:
    data = json.load(f)


print("-" * 80)
for k in data:
    print(k)
print("-" * 80)
