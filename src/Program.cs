using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ImportTest
{
    class Program
    {
        static void Main(string[] args)
        {
            Stopwatch sw = Stopwatch.StartNew();

            var cnn = new SqlConnection("Trusted_Connection=Yes;data source=.\\sqlexpress;initial catalog=teste");
            cnn.Open();
            using (cnn)
            {
                using (var tran = cnn.BeginTransaction(System.Data.IsolationLevel.Serializable))
                {
                    ExecuteCommand(tran, @"SELECT TOP 0 Id, Name, Description, CAST(NULL AS XML) Images INTO #ProductTemp  FROM Product 
CREATE UNIQUE CLUSTERED INDEX PK_PRODUCTTEMP ON #ProductTemp(Id)");

                    var reader = new EnumerableDataReader<Product>(GenerateProducts(1000 * 1000));                   

                    reader
                        .AddColumn(i => i.Id)
                        .AddColumn(i => i.Name)
                        .AddColumn(i => i.Description)
                        .AddColumn(i => GetImagesXml(i.Images));

                    var bcp = new SqlBulkCopy(cnn, SqlBulkCopyOptions.Default, tran);

                    bcp.DestinationTableName = "#ProductTemp";
                    bcp.BulkCopyTimeout = 60 * 10;
                   
                    Console.Write("Insert into temp table...");
                    bcp.WriteToServer(reader);
                    Console.WriteLine(" Done! ({0} seconds)" , sw.Elapsed.TotalSeconds);

                    sw = Stopwatch.StartNew();
                    Console.Write("Merging into product table...");

                    MergeProducts2(tran);

                    Console.WriteLine(" Done! ({0} seconds)", sw.Elapsed.TotalSeconds);

                    tran.Commit();
                }
            }            
        }

        private static void MergeProducts1(SqlTransaction tran)
        {
            ExecuteCommand(tran, @"
MERGE ProductImage AS p
USING (SELECT temp.Id ProductId, img.value('@idx', 'int') Idx, img.value('@url', 'nvarchar(max)') Url FROM #ProductTemp temp CROSS APPLY temp.Images.nodes('/images/img') t(img) ) 
AS src (ProductId, Idx, Url)
ON p.ProductId = src.ProductId AND p.Idx = src.Idx
WHEN MATCHED
    THEN  
		UPDATE SET p.Url = src.Url, p.Idx = src.Idx
WHEN NOT MATCHED BY TARGET
    THEN  
		INSERT (ProductId, Idx, Url) VALUES (src.ProductId, src.Idx, src.Url)
WHEN NOT MATCHED BY SOURCE
    THEN DELETE;



MERGE Product AS p
USING (SELECT Id, Name, Description 
        FROM #ProductTemp AS t ) AS src (Id, Name, Description)
ON p.Id = src.Id
WHEN MATCHED
    THEN  
		UPDATE SET p.Name = src.Name
	WHEN NOT MATCHED BY TARGET
    THEN  
		INSERT (Id, Name, Description) VALUES (Id, Name, Description)
WHEN NOT MATCHED BY SOURCE
    THEN DELETE;
");
        }

        private static void MergeProducts2(SqlTransaction tran)
        {
            ExecuteCommand(tran, @"
DELETE ProductImage
DELETE Product

INSERT INTO Product(Id, Name, Description)
SELECT Id, Name, Description
FROM #ProductTemp t

INSERT INTO ProductImage(ProductId, Url, Idx)
SELECT temp.Id, img.value('@url', 'nvarchar(max)'), img.value('@udx', 'int')
FROM #ProductTemp temp
CROSS APPLY temp.Images.nodes('/images/img') t(img)
");
        }

        private static void ExecuteCommand(SqlTransaction tran, string command)
        {
            var cmd = new SqlCommand(command, tran.Connection, tran);
            cmd.CommandTimeout = 10 * 60;
            cmd.ExecuteNonQuery();
        }

        private static string GetImagesXml(IList<ProductImage> images)
        {
            StringBuilder sb = new StringBuilder("<images>");
            foreach (var img in images)
                sb.AppendFormat("<img url=\"{0}\" idx=\"{1}\" />", img.Url, img.Idx);
            sb.Append("</images>");
            return sb.ToString();
        }

        static IEnumerable<Product> GenerateProducts(int count)
        {
            Random rnd = new Random();
            int currentId = 1;

            for (int i = 0; i < count; i++)
                yield return Product.Random(rnd, (currentId += rnd.Next(1, 5)));
        }
    }
}
