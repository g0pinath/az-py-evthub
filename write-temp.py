import random
import string

# printing lowercase
letters = string.ascii_lowercase


filename=''.join(random.choice(letters) for i in range(10))+".txt"
print(filename)