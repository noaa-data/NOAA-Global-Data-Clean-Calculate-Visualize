## Intro
- From raw data hosted by NOAA to cleaned, consolidated, and calculated data in PostgreSQL/PostGIS on Heroku.
- Why?
    - I'm interested in the natural world, environmental sciences, and climate change.
    - My original formal education is in the liberal arts. I have a bachelor's degree in theology (with as much philosophy as I could fit in).
    - A few years after my undergrad my interests shifted. I did 4 or 5 additional college math classes, some basic science classes, a geography course, a phyrical geography course, a couple of programming courses, and moved into a programming role in the college IT department I work for. I also spent several hours a week every Friday for a year a little while back voluntaring as a Soil Science Lab Assistant at the local arbortetum outside of Chicago (Mortom Arboretum).
    - Originally I thought I might pursue the prerequisites to enter a master's program for soil science or perhaps physical geography. But eventually decided that I also genuinely love programming and computer science concepts. What really helped my decision to continue on the technical path was the realization that I could use those technical skills to support work done in the environment sciences. I saw what people were doing with data analysis or data science, spatial data, mapping, remote sensing, data streaming, and it was apparent that was a great fit for my skillsets and intersts.
- Why these items for this project?
    - Well, when I looked at the options for what the final project can be based on several of them looked like what would be a part of a full workflow that would exist in the real world.
    - And the idea of "real world" is key. I've worked in IT for about 10 years, and I've been a Systems Analyst on a team that manages a college ERP for close the 3 years. 
    - When I see really neat ideas, or follow some kind of tutorial or walkthrough for an isolated concept I tend to think, "That's really neat, but what does it actually look like to use that in the real world?"
    - I considered Working with something related to spatial database programming, spatial queries, spatial visualization, or working with "big data." In the end it seemed like all of these concepts would be used in a real-world process to get data from point A to point B.
    - In this case, point A would be the original point of raw data -- maybe directly from sensors in the field, but at least hosted in closer to raw form -- which would mean less than perfet structure, as well as not free of mistakes or bad data.
    - Point B would be the end product. Data that is ready for use for analysis, visulization, or a combination of both.
    - In the end I decided to try and tackle a working concept of the entir A to B process. Acquiring large quantities of unprocessed data, move that data to AWS S3 cloud storage, clean the data, do some calculations to transform the data into a different format or structure, load the data into a spatial database with a spatial column, add some spatial indexing, then execute queries on that final data, as well as visualize it in QGIS and run queries there as well.
- Why did I choose global temperature data?
    - Using global temperature data for my project seemed like the nature choice. I have always been curious about how large-ish quantities of data are handled, and how those data sets get into a structure that can be represented and analyized on maps.
    - I found the pages containing the NOAA dataset I used a couple of years ago. I originally dowloaded all of the data with rudimentary script running on a Raspberry Pi, but never really did much with it. This project seemed like a great reason to finally use the data that orginally interested me so much anyway. (and a good reason to reinvent my download script from scratch).
    - I'm well aware that in many ways I've reivented the wheel here, but it's also usually pretty difficult to do new, unique, and creative work, without first learning the basic structures that creative work has to stand on.
    - In the end, doing the A to B process myself of getting a decent sized set of raw data (~35gb of plain text), and tranforming it into data that can be efficiently spatially viewed and analyized was satisfying.
    - The experience has also generated many other ideas of what to do with data like this in general, as well as some ideas about this data specifically.

## Technologies Used

### Prefect

### Coiled

### AWS

### Heroku

### Dataset
- Website
- Directories on Server (terminal command to show number of files in 2020; terminal command to show number of files in root file directory; terminal command to show size of dataset)

## Technologies I had to Learn
- AWS S3
  - S3 (never used before)
  - CLI (never used before) (mostly for authenticating and iteratively deleting large numbers of files while performing trial and error)
- boto3 (never used before)
  - Creating objects (files)
  - Reading objects
  - Copying objects
  - Deleting objects
  - Updating object metadata ("lastmodified")
- Adding PostGIS plugin to Heroku
  - Used Heroku and postgres on it in the past, but never had to deal with figuring out how to manage open connections when doing parallel work -- "basic" version limits open connections to 20)
- Coiled (never used before)
- QGIS (only a little previous usage)
- GDAL (mostly to convert geojson to a table in postgres)
