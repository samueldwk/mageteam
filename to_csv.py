import xlrd
import csv

# Input XLS file path
xls_file = rf"C:\Users\Mage Team\Documents\m&p\Relatorios Diarios\2023-12-14\ajobrand_Produtos_2023-12-14.xls"

# Output CSV file path
csv_file = rf"C:\Users\Mage Team\Documents\m&p\Relatorios Diarios\2023-12-14\ajobrand_Produtos_2023-12-14.csv"

# Open the XLS file
workbook = xlrd.open_workbook(xls_file)

# Select the first sheet (you can change this to select a specific sheet if needed)
sheet = workbook.sheet_by_index(0)

# Open a CSV file for writing
with open(csv_file, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    
    # Iterate through the rows in the sheet and write them to the CSV file
    for row_num in range(sheet.nrows):
        row = sheet.row_values(row_num)
        csv_writer.writerow(row)

print(f"Conversion completed. CSV file saved as {csv_file}")

