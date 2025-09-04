import numpy as np
from scipy.stats import chisquare
def benford_pval(values):
    arr=np.array(values,dtype=float); arr=arr[np.isfinite(arr)&(arr>0)]
    if arr.size<20: return 0.5
    first_digits=[]
    for x in arr:
        s=str(int(abs(x)))
        if s and s[0].isdigit() and s[0] != '0': first_digits.append(int(s[0]))
    if not first_digits: return 0.5
    obs=np.bincount(first_digits,minlength=10)[1:10]
    exp=np.log10(1+1/np.arange(1,10))
    return float(chisquare(obs,f_exp=obs.sum()*exp)[1])
