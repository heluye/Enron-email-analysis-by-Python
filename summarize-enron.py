import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import argparse
import sys


if __name__ == "__main__":

	path = sys.argv[1]

	#import data
	df = pd.read_csv(path, header = None)

	#asign column names
	df.columns = ['time','message identifier','sender','recipients','topic','mode']

	#convert time format and add a new time column with granualar to specific month
	df['real time']=pd.to_datetime(df['time'],unit='ms').dt.to_period('M')


	##Part1
	#count total emails sent by each person
	sender_stats = df[['sender','message identifier']].groupby(['sender']).count().reset_index()
	sender_stats= sender_stats.sort_values(by=['message identifier'],ascending=False).rename(columns={'message identifier':'sent','sender':'person'})

	#seperate recipients
	df['recipient']=df['recipients'].str.split('|')

	#list seperated recipients in additional rows
	df_new = df.explode('recipient')

	#count total emails received by each person
	recipient_stats = df_new[['recipient','message identifier']].groupby(['recipient']).count().reset_index()
	recipient_stats = recipient_stats.sort_values(by=['message identifier'],ascending=False).rename(columns={'message identifier':'received', 'recipient':'person'})


	#get total sent and received emails by each person
	final = sender_stats.merge(recipient_stats,how='outer',on='person').sort_values(by=['sent'],ascending=False).fillna(0)
	final.to_csv('email_by_person.csv')




	##Part 2
	#get top five sender
	top_sender = final['person'][0:5].values.tolist()

	#filter records for top 5 senders
	sender_df = df.loc[df['sender'].isin(top_sender), ['real time','sender','message identifier']]

	#counts total emails sent by each of top 5 sender by month
	sender_df_agg = sender_df.groupby(['sender','real time']).count().rename(columns={'message identifier':'counts'}).reset_index()

	#plot by each sender
	fig, ax = plt.subplots(figsize=(16,8))
	for label, subgroup in sender_df_agg.groupby('sender'):
	    subgroup.plot(x='real time', y='counts', ax=ax, label=label,title='Emails Sent by the Top 5 Senders')
	plt.legend()
	ax.set_xlabel("Time Line")
	ax.set_ylabel("Total Email Counts")

	#save results to png file
	fig.savefig('Emails Sent by the Top 5 Senders.png')



	## Part 3
	#filter emails received by the top 5 senders
	receive_df = df_new.loc[df_new['recipient'].isin(top_sender), ['real time','recipient','sender']].reset_index(drop=True)

	#count unique senders who sent emails to each of top 5 senders each month
	receive_df_agg = pd.DataFrame(receive_df.groupby(['real time','recipient'])['sender'].nunique()).reset_index()

	#calculate the sum of total unique senders by month
	receive_sum = receive_df_agg.groupby(['real time']).sum().rename(columns={'sender':'sum'}).reset_index()

	#merge the two datasets together
	receive_df_agg = receive_df_agg.merge(receive_sum, how='left',on='real time')

	#create a new column to measure the proportion of the each sender 
	receive_df_agg['proportion'] = receive_df_agg['sender']/receive_df_agg['sum']*100

	#pivot table for visualization
	receive_df_agg_pivot = receive_df_agg.pivot(index = 'real time',columns = 'recipient',values= 'proportion').fillna(0)

	#plot the results
	fig2, ax2 = plt.subplots(figsize=(16,8))

	receive_df_agg_pivot.plot.bar(stacked=True, title = 'Percentage of People Who Contacted the Top 5 Senders', ax=ax2)
	ax2.set_xlabel("Time Line")
	ax2.set_ylabel("Relative Number of People as Percentage")

	#save as PNG file
	fig2.savefig('Percentage of People Who Contacted the Top 5 Senders.png')


	print('mission complete')

