using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.IO.Abstractions;

using Dicom;
using MongoDB.Driver;
using MongoDB.Bson;

namespace MongoDICOMTester
{
    class MongoDBDICOMTester
    {

        static readonly DataTable FILES_TO_LOAD = new DataTable("ImagesToLoadList");
        static readonly string IMAGE_LOCATION_COLUMN_LABEL = "FilePath";

        static void Main(string[] args)
        {

            //TestMongoWrite().Wait();

            TestRead().Wait();

            TestDICOMFormat().Wait();
        }

        /// <summary>
        /// Test that we can write some data to MongoDB
        /// </summary>
        /// <returns></returns>

        static async Task TestMongoWrite()
        {

            var mongoConnectionString = @"mongodb://localhost:27017";

            Console.WriteLine("Testing we can write to Mongo");
            Console.WriteLine("Attempting to connect to local mongod instance on: " + mongoConnectionString);

            try
            {

                var mongoClient = new MongoClient(mongoConnectionString);

                IMongoDatabase db = mongoClient.GetDatabase("TEST");
                IMongoCollection<BsonDocument> collection = db.GetCollection<BsonDocument>("students");

                var document = new BsonDocument
                {
                  {"firstname", BsonValue.Create("Peter")},
                  {"lastname", new BsonString("Mbanugo")},
                  { "subjects", new BsonArray(new[] {"English", "Mathematics", "Physics"}) },
                  { "class", "JSS 3" },
                  { "age", int.MaxValue }
                };

                await collection.InsertOneAsync(document);

                Console.WriteLine("Written document to mongo");
            }
            catch (Exception e)
            {

                Console.WriteLine("Error attempting to connect to server: \n" + e.Message);
            }


            Console.WriteLine("Connected");
            //createTestCollection();


            Console.WriteLine("--- Ends ---");
            Console.ReadLine();
        }


        /// <summary>
        /// Test we can read dicom files.
        /// 
        /// Scans directories recursively from the root and adds all dcm files to a DataTable.
        /// </summary>
        /// <returns></returns>
        static async Task TestRead()
        {
            // From: SMIPlugin/SMIPlugin/PipelineComponents/DicomFileFinder.cs


            //TODO(Ruairidh): Add recursion for subdirectories and a proper stack to search
            //See: https://github.com/HicServices/SMIPlugin/blob/496317f5110306e998bfc238ff53d467a0637b9a/SMIPlugin/PipelineComponents/DicomFileFinder.cs#L105

            //TODO(Ruairidh): Add logging for finding DICOM files

            Console.WriteLine("\n:: Testing we can locate all DICOM files ::\n");
            

            DataColumn column = new DataColumn();
            column.DataType = System.Type.GetType("System.String");
            column.ColumnName = IMAGE_LOCATION_COLUMN_LABEL;

            FILES_TO_LOAD.Columns.Add(column);

            // Search path for DICOM files
            string path = Path.GetDirectoryName(Path.GetDirectoryName(System.IO.Directory.GetCurrentDirectory())) + @"\Sample DICOM\";

            // Stack of directories to search
            var dirStack = new Stack<string>();
            dirStack.Push(path);

            IFileSystem fileSystem = new FileSystem();
            int MINIMUM_CHUNK_THRESHOLD = 1000;

            //string[] files = Directory.GetFiles(path);

            DataRow row;

            while(dirStack.Count > 0)
            {

                string dir = dirStack.Pop();

                try
                {
                    if (fileSystem.Directory.Exists(dir))
                    {
                        // Add subdirectories to the list of directories to explore
                        foreach (string subdir in fileSystem.Directory.GetDirectories(dir))
                        {
                            dirStack.Push(subdir);
                        }

                        // Add the files to the data table one per row
                        foreach (string dicomFile in fileSystem.Directory.EnumerateFiles(dir, "*.dcm"))
                        {
                            row = FILES_TO_LOAD.NewRow();
                            row[IMAGE_LOCATION_COLUMN_LABEL] = dicomFile;
                            FILES_TO_LOAD.Rows.Add(row);
                            //Console.WriteLine("Each Row: " + row[IMAGE_LOCATION_COLUMN_LABEL]);                 
                        }

                        // If result is getting big then return this chunk
                        if (FILES_TO_LOAD.Rows.Count >= MINIMUM_CHUNK_THRESHOLD)
                        {
                            //_logger.FinishedDirectory(dir);
                            //_logger.FinishedChunk();
                            //return filesToLoad;
                        }
                    }
                    else if (fileSystem.File.Exists(dir))
                    {
                        //_logger.SpecifiedDirectoryIsNotADirectory(dir);
                    }
                    else
                    {
                        //_logger.SpecifiedDirectoryDoesNotExist(dir);
                    }
                }
                catch (IOException e)
                {
                    //_logger.IOException(dir, e);
                    Console.WriteLine(e.Message);
                }
            }

            // Should now have DataTable of rows to read into DICOM format
            Console.WriteLine("No of files: " + FILES_TO_LOAD.Rows.Count);

            //Console.WriteLine("--- Ends ---");
            //Console.ReadLine();
        }

        /// <summary>
        /// Test we can load dicom images into memory using the fo-dicom lib.
        /// </summary>
        /// <returns></returns>
        static async Task TestDICOMFormat()
        {
            
            Console.WriteLine("\n:: Testing we can read DICOM file tags ::\n");
            for (int i = 0; i <  FILES_TO_LOAD.Rows.Count; i++)
            {
                string filePath = (string)FILES_TO_LOAD.Rows[i][IMAGE_LOCATION_COLUMN_LABEL];
                var file = await DicomFile.OpenAsync(filePath);

                var patientId = file.Dataset.Get<string>(DicomTag.PatientID);
                var seriesDescription = file.Dataset.Get<string>(DicomTag.SeriesDescription);
                var studyDescription = file.Dataset.Get<string>(DicomTag.StudyDescription);
                var seriesNumber = file.Dataset.Get<string>(DicomTag.SeriesNumber );

                Console.WriteLine();
                Console.WriteLine("Event: " + i);
                Console.WriteLine("Reading from file: " + filePath);
                Console.WriteLine("patientId in file is: " + patientId);
                Console.WriteLine("SeriesDescription in file is: " + seriesDescription);
                Console.WriteLine("StudyDescription in file is: " + studyDescription);
                Console.WriteLine("Series Number: " + seriesNumber);
            }
            Console.WriteLine("\n--- Ends here! ---\n\nReturn to exit...");
            Console.ReadLine();
        }
    }
}
