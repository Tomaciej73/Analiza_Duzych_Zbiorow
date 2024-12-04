# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt
import numpy as np
response_codes = [1,2,3]
response_code_counts = [10,5,6]
Labels = [200,404,300]
#print(response_codes)
#print(response_code_counts)
plt.bar(response_codes,response_code_counts)
plt.xticks(response_codes,Labels,fontsize=5)
plt.xlabel('Zwrócony kod HTTP z serwera')
plt.ylabel('Ilość')
plt.title('Analiza zwróconych kodów HTTP')
plt.show()