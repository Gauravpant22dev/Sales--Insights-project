import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('mysql+pymysql://root:1234567890@localhost/internship_project')
print(engine)
df = pd.read_csv("sales.csv").copy()

# new table gender
percentage_of_gender = df['Gender'].value_counts(normalize=True).reset_index()
percentage_of_gender.columns=['Gender','Percentage']
percentage_of_gender['Percentage'] = percentage_of_gender['Percentage']*100
percentage_of_gender.to_csv('gender_percentage.csv',index=False)

# newtableofcategory

count_of_category = df['Category'].value_counts().reset_index()
count_of_category.columns=['Category','Category_Total_Sales']
count_of_category.to_csv('category_count.csv',index=False)


#coloumn
new = df.groupby('Category')['Amount'].sum()
category_amount = new.reset_index()
category_amount.columns=['Category','Category_amount']
category_amount.to_csv('amount_category.csv',index=False)
# 

# print(df.info())
#paymethod
payment_method = df.groupby('PaymentMethod')['CustomerID'].count()
payment_method=payment_method.reset_index()
payment_method.to_csv('Payment_method.csv',index=False)
# print(df.info())


# itemrating
df['ProductTier'] = 'Unknown'
df.loc[df['ItemRating']>=4.5,'ProductTier']='Top Tier'
df.loc[(df['ItemRating']>=3.0 )& (df['ItemRating']<=4.5),'ProductTier']='Mid Tier'
df.loc[df['ItemRating']<3.0 ,'ProductTier']='Low Tier'
itemrating=df['ProductTier']
# itemrating.reset_index(drop=True)
# itemrating.columns = ['Product Tier','Item Rating']



#Loyalty
# print(df['PreviousPurchases'].head(60))
df['LoyaltyStatus']= 'Unknown'
df.loc[df['PreviousPurchases']>=10,'LoyaltyStatus']='MaxLoyalBuyer'
df.loc[(df['PreviousPurchases']>=8)&(df['PreviousPurchases']<10),'LoyaltyStatus']='LoyalBuyer'
df.loc[(df['PreviousPurchases']>=4)&(df['PreviousPurchases']<8),'LoyaltyStatus']='Buyer'
df.loc[df['PreviousPurchases']<4,'LoyaltyStatus']='NewBuyers'
Loyalty = df['LoyaltyStatus']
# BuyerStaus=df[['PreviousPurchases','LoyaltyStatus']]
# BuyerStaus=BuyerStaus.reset_index(drop=True)
# BuyerStaus.columns = ['Previous Purchase','Loyalty Status']


# i have gender percentage table, categorywise amount table, categorywise_count ,payment method count, itemrating , loyalty. Ab isse kya bn skta h sochna h. 
# concant tables
gender = df['Gender'].reset_index(drop=True)
rating = itemrating.reset_index(drop=True)
loyal_customer = Loyalty.reset_index(drop=True)

final_df = pd.concat([gender,rating,loyal_customer],axis=1)
final_df .columns=['Gender','Rating_tier','Loyal_customer']
# print(final_df)
# final_df.to_csv('sales_concat.csv',index=False)

# Sending data to mysql

try:
    final_df.to_sql('sales_summary',con=engine,if_exists='replace',index=False)
    print("data chle gya")
except Exception as e:
    print(f'error:{e}')