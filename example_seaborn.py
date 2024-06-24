import seaborn as sns
import matplotlib.pyplot as plt

tips = sns.load_dataset("tips")
print(tips.head())

# 繪製散佈圖
# sns.scatterplot(x='total_bill', y='tip', data=tips)

# 繪製箱型圖
# sns.boxplot(x='day', y='total_bill', data=tips)

# 繪製直方圖
# sns.histplot(tips['total_bill'], bins=30)

# 子圖
g = sns.FacetGrid(tips, col="time", row="sex")
g.map(sns.scatterplot, "total_bill", "tip")

# 保存圖形為 PNG 檔案
plt.savefig("results.png")
