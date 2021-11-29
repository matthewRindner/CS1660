import tkinter as tk

def main():
    print("hi")
    root = tk.Tk()
    root.title('CS1660 Project 1 GUI Driver')
    root['background']='yellow'
    root.geometry('600x200')

    label = tk.Label(root, text="Welcome to the Big Data Processing Toolbox", font= ('Aerial 17 bold underline'))
    label.pack()

    hadoop_button = tk.Button(root, text='Apache Hadoop', command=open_hadoop)
    hadoop_button.pack(side='top')


    spark_button = tk.Button(root, text='Apache Spark', command=open_spark)
    spark_button.pack(side='top')

    jupyter_button = tk.Button(root, text='Jupyter Notebook', command=open_jupyter)
    jupyter_button.pack(side='top')

    sonar_button = tk.Button(root, text='SonarQube and SonarScanner', command=open_sonar)
    sonar_button.pack(side='top')

    root.mainloop()

def open_hadoop():
    pass

def open_spark():
    pass

def open_jupyter():
    pass

def open_sonar():
    pass

if __name__ == '__main__':
    main()