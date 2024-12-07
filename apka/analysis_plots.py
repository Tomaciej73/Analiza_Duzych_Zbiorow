import matplotlib.pyplot as plt
import numpy as np
import pandas as pd 
from collections import OrderedDict

def autolabel(rects):
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width()/2., 1.05*height,
                '%d' % int(height),
                ha='center', va='bottom')

def content_analysis_plot(content_size):
    counts = [1,2,3]
    LABELS = ["AVG rozmiar","MIN rozmiar","MAX rozmiar"]
    plt.bar(counts,content_size)
    plt.xticks(counts,LABELS)
    plt.show()

def bar_plot_list_of_tuples(data, x_label, y_label, plot_title):
    """
    Tworzy pionowy wykres słupkowy z listy krotek.
    """
    labels, values = zip(*data)

    values = pd.to_numeric(values, errors='coerce')
    valid_indices = ~pd.isnull(values)

    labels = [labels[i] for i in range(len(labels)) if valid_indices[i]]
    values = [v for v in values if not pd.isnull(v)]

    plt.figure(figsize=(10, 6))
    plt.bar(labels, values)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(plot_title)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()

def bar_plot_list_of_tuples_horizontal(data, x_label, y_label, plot_title, max_label_length=30):
    """
    Tworzy poziomy wykres słupkowy z listy krotek.
    """
    labels, values = zip(*data)

    values = pd.to_numeric(values, errors='coerce')
    valid_indices = ~pd.isnull(values)

    labels = [labels[i] for i in range(len(labels)) if valid_indices[i]]
    values = [v for v in values if not pd.isnull(v)]
    labels = [label if len(label) <= max_label_length else label[:max_label_length] + "..." for label in labels]
    max_value = int(np.ceil(max(values) / 100.0) * 100)
    plt.figure(figsize=(12, 7))
    plt.barh(labels, values, color="skyblue")
    plt.ylabel(y_label, fontsize=10)
    plt.xlabel(x_label, fontsize=10)
    plt.title(plot_title, fontsize=12)
    plt.xticks(range(0, int(max_value) + 1, 100))
    plt.tight_layout()
    if len(labels) > 10:
        plt.yticks(fontsize=8)
    else:
        plt.yticks(fontsize=10)
    plt.show()

def time_series_plot(data, x_label, y_label, plot_title):
    """
    Tworzy wykres szeregów czasowych.
    """
    labels, values = zip(*data)

    values = pd.to_numeric(values, errors='coerce')
    valid_indices = ~pd.isnull(values)

    labels = [labels[i] for i in range(len(labels)) if valid_indices[i]]
    values = [v for v in values if not pd.isnull(v)]

    plt.figure(figsize=(12, 6))
    plt.plot(labels, values, marker='o')
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(plot_title)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()