import pandas as pd
import matplotlib.pyplot as plt
from pandas.plotting import table

def read_data(file_path):
    # Assuming the data is tab-separated; adjust separator if needed
    return pd.read_csv(file_path, sep=',', header=None, names=['Threads for Hash Gen', 'Threads for Sorting', 'Threads for Writing', 'Time'])


def main():
    # Read data for 1GB files
    df = read_data('time1.txt')

    # Analysis 1: Impact of threads for hash generation
    df_hash_gen = df.groupby('Threads for Hash Gen')['Time'].mean().reset_index()
    plt.figure(figsize=(8, 5))
    plt.plot(df_hash_gen['Threads for Hash Gen'], df_hash_gen['Time'], marker='o')
    plt.xlabel('Threads for Hash Generation')
    plt.ylabel('Average Time Taken (s)')
    plt.title('Impact of Threads for Hash Generation on Time Taken')
    plt.grid(True)
    plt.savefig("Gen_Graph_1",dpi=800)
    
    # Analysis 2: Impact of threads for sorting
    df_sorting = df.groupby('Threads for Sorting')['Time'].mean().reset_index()
    plt.figure(figsize=(8, 5))
    plt.plot(df_sorting['Threads for Sorting'], df_sorting['Time'], marker='o', color='green')
    plt.xlabel('Threads for Sorting')
    plt.ylabel('Average Time Taken (s)')
    plt.title('Impact of Threads for Sorting on Time Taken')
    plt.grid(True)
    plt.savefig("Sort_Graph_1",dpi=800)
    
    # Analysis 3: Impact of threads for writing
    df_writing = df.groupby('Threads for Writing')['Time'].mean().reset_index()
    plt.figure(figsize=(8, 5))
    plt.plot(df_writing['Threads for Writing'], df_writing['Time'], marker='o', color='red')
    plt.xlabel('Threads for Writing')
    plt.ylabel('Average Time Taken (s)')
    plt.title('Impact of Threads for Writing on Time Taken')
    plt.grid(True)
    plt.savefig("Write_Graph_1",dpi=800)
    
    # Read data for 64GB files
    df = read_data('time64.txt')

    # Analysis 1: Impact of threads for hash generation
    df_hash_gen = df.groupby('Threads for Hash Gen')['Time'].mean().reset_index()
    plt.figure(figsize=(8, 5))
    plt.plot(df_hash_gen['Threads for Hash Gen'], df_hash_gen['Time'], marker='o')
    plt.xlabel('Threads for Hash Generation')
    plt.ylabel('Average Time Taken (s)')
    plt.title('Impact of Threads for Hash Generation on Time Taken')
    plt.grid(True)
    plt.savefig("Gen_Graph_64",dpi=800)
    
    # Analysis 2: Impact of threads for sorting
    df_sorting = df.groupby('Threads for Sorting')['Time'].mean().reset_index()
    plt.figure(figsize=(8, 5))
    plt.plot(df_sorting['Threads for Sorting'], df_sorting['Time'], marker='o', color='green')
    plt.xlabel('Threads for Sorting')
    plt.ylabel('Average Time Taken (s)')
    plt.title('Impact of Threads for Sorting on Time Taken')
    plt.grid(True)
    plt.savefig("Sort_Graph_64",dpi=800)
    
    # Analysis 3: Impact of threads for writing
    df_writing = df.groupby('Threads for Writing')['Time'].mean().reset_index()
    plt.figure(figsize=(8, 5))
    plt.plot(df_writing['Threads for Writing'], df_writing['Time'], marker='o', color='red')
    plt.xlabel('Threads for Writing')
    plt.ylabel('Average Time Taken (s)')
    plt.title('Impact of Threads for Writing on Time Taken')
    plt.grid(True)
    plt.savefig("Write_Graph_64",dpi=800)

if __name__ == '__main__':
    main()
