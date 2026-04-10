from PIL import Image, ImageDraw

size = 256
img = Image.new('RGBA', (size, size), (0, 0, 0, 0))
d = ImageDraw.Draw(img)

# body
d.rounded_rectangle((36, 70, 220, 210), radius=42, fill=(108, 193, 120, 255), outline=(54, 129, 66, 255), width=8)
# eyes
d.ellipse((86, 108, 112, 134), fill=(255, 255, 255, 255))
d.ellipse((144, 108, 170, 134), fill=(255, 255, 255, 255))
d.ellipse((95, 116, 104, 125), fill=(34, 34, 34, 255))
d.ellipse((153, 116, 162, 125), fill=(34, 34, 34, 255))
# smile
d.arc((96, 132, 160, 172), start=10, end=170, fill=(34, 34, 34, 255), width=5)
# chart bars
d.rounded_rectangle((70, 150, 92, 188), radius=6, fill=(63, 81, 181, 255))
d.rounded_rectangle((102, 136, 124, 188), radius=6, fill=(33, 150, 243, 255))
d.rounded_rectangle((134, 120, 156, 188), radius=6, fill=(255, 193, 7, 255))
d.rounded_rectangle((166, 96, 188, 188), radius=6, fill=(244, 67, 54, 255))
# tiny antenna leaf
d.line((126, 70, 126, 40), fill=(54, 129, 66, 255), width=8)
d.ellipse((112, 22, 148, 52), fill=(129, 199, 132, 255), outline=(54, 129, 66, 255), width=4)

img.save('C:/Users/Administrator/Desktop/sql-tool/assets/app_icon.png')
img.save('C:/Users/Administrator/Desktop/sql-tool/assets/app_icon.ico')
