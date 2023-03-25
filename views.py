from django.shortcuts import render
from django.http import HttpResponse
import os
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt1
import pandas as pd
import pymysql
import matplotlib
import subprocess
matplotlib.use('Agg')
import time
import random
import uuid
from PIL import Image
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import numpy as np
import time 
from django.core.cache import cache



def index(request):
    return render(request, 'insightlyze.html')


def analyze(request):
    if request.method == 'POST':
        # Get the form data
        client = request.POST['client']
        event_type = request.POST['event-type']
        period = request.POST['period']

        if event_type == 'Custom Search':
            sender = request.POST['sender']
            custom_text = request.POST['custom-text']
            script_path = "Insightlyze/ngrams.py"
            #result = subprocess.run(["python", script_path, client, event_type, period, custom_text, sender], capture_output=True, text=True)
            #if result.returncode == 0:
                #print("Script executed successfully.")
                #print("Output:", result.stdout)
            #else:
                #print("Script execution failed.")
                #print("Error:", result.stderr)
        else:
            sender = 'NA'
            custom_text = 'NA'
            script_path = "Insightlyze/ngrams.py"
            #result = subprocess.run(["python", script_path, client, event_type, period], capture_output=True, text=True)
            #if result.returncode == 0:
                #print("Script executed successfully.")
                #print("Output:", result.stdout)
            #else:
                #print("Script execution failed.")
                #print("Error:", result.stderr)

        # Connect to the MySQL database
        connection = pymysql.connect(host='xx',
                                     user='xx',
                                     password='xx',
                                     database='test_sbg')

        try:
            with connection.cursor() as cursor:
                # Insert the data into the table
                sql = "INSERT INTO `sona_ngrams_user_inputs` (`client`, `event_type`, `period`, `sender`, `custom_text`) VALUES (%s, %s, %s, %s, %s)"
                cursor.execute(sql, (client, event_type, period, sender, custom_text))

            # Commit the transaction
            connection.commit()

        finally:
            # Close the connection
            connection.close()
        context = {
            'client': client,
            'event_type': event_type,
            'period': period,
            'sender': sender,
            'custom_text': custom_text
        }
        dir_path = "D:/project/Insightlyze/Insightlyze/static"
        for filename in os.listdir(dir_path):
            if filename.endswith(".png"):
                os.remove(os.path.join(dir_path, filename))

        return render(request, "analysis.html", context)
    else:
        return HttpResponse("Error")


def word_cloud_csv(request, file_path):
    cache.delete('word_cloud_csv')
    df = pd.read_csv(file_path, header=None, names=['Word', 'FontSize'])
    print('file_path')
    print(file_path)
    # Filter out words with font size less than 10
    df = df[df['FontSize'] >= 10]

    # Create the word cloud
    word_cloud = WordCloud(width=800, height=800, background_color='white', max_font_size=100).generate_from_frequencies(df.set_index('Word')['FontSize'].to_dict())

    # Plot the word cloud
    plt.figure(figsize=(8, 8), facecolor=None)
    plt.imshow(word_cloud)
    plt.axis("off")
    plt.tight_layout(pad=0)
    # Save the plot to a file
    file_name, extension = os.path.splitext(file_path)
    number = file_name.split('_')[1]
    img_path = f"static/wordcloud_{number}.png"
    plt.savefig(os.path.join("Insightlyze", img_path))
    plt.clf()
    plt.close()

    # Send the image as a response
    with open(os.path.join("Insightlyze", img_path), 'rb') as f:
        response = HttpResponse(f.read(), content_type="image/png")
        response['Content-Disposition'] = 'inline; filename=' + os.path.basename(img_path)
        return response


from datetime import datetime

def histogram_csv(request,file_path):
    cache.delete('histogram_csv')
    df = pd.read_csv(file_path)
    print(file_path,'file_path')
    # Create the histogram plot
    plt1.bar(df['Bucket'], df['Rec_Count'])
    plt1.xlabel('Bucket')
    plt1.xticks(rotation=45)
    plt1.tight_layout()
    # Remove y-axis
    plt1.gca().axes.get_yaxis().set_visible(False)
    # Save the plot to a file
    hist_img_path = f"static/histogram.png"
    plt1.savefig(os.path.join("Insightlyze", hist_img_path))
    plt1.clf()
    plt1.close()

    # Send the image as a response
    with open(os.path.join("Insightlyze", hist_img_path), 'rb') as f:
        response = HttpResponse(f.read(), content_type="image/png")
        response['Content-Disposition'] = 'inline; filename=' + os.path.basename(hist_img_path)
        return response
