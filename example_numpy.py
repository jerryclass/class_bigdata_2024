import numpy as np

# 建立兩個一維陣列
a = np.array([10, 20, 30, 40])
b = np.array([1, 2, 3, 4])

print(f"a={a}")
print(f"b={b}")

print("--陣列的基礎四則運算--")
print(f" {a} + {b} = {a + b}")
print(f" {a} - {b} = {a - b}")
print(f" {a} * {b} = {a * b}")
print(f" {a} / {b} = {a / b}")

print("--陣列常見的函數運算--")
print(f"sum(a) = {np.sum(a)}")
print(f"avg(a) = {np.mean(a)}")
print(f"std(a) = {np.std(a)}")
print(f"max(a) = {np.max(a)}")
print(f"min(a) = {np.min(a)}")

# 建立兩個矩陣
a = np.array([[1, 2], [3, 4]])
b = np.array([[5, 6], [7, 8]])
print(f"a=\n {a}")
print(f"b=\n {b}")

print("--矩陣的基礎四則運算--")
print(f" a + b = \n {a + b}")
print(f" a - b = \n {a - b}")
print(f" dot(a, b) = \n {np.dot(a, b)}")
print(f" transpose(a) = \n {np.transpose(a)}")
print(f" inv(a) = \n {np.linalg.inv(a)}")