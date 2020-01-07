from  datetime import datetime as dt

lista = []

for x in range(100000):
    error = x & 2 == 0
    lista.append({"error": error, "value": x}) 

print(len(lista))

initial_time = dt.now()
errors = 0
success = 0
sum_values = 0

## Opcion 1
# for i in lista:
#     if i["error"]: errors += 1
    # else: success += 1
    # sum_values += i["value"]

## Opcion 2
l = len(lista)
errors = ([i["error"] for i in lista]).count(True)
success = l - errors
sum_values = sum([i["value"] for i in lista])

elapsed_time = dt.now() - initial_time
print(elapsed_time)
print(errors, success, sum_values)