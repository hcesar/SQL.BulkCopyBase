using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ImportTest
{
    class Product
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }

        public IList<ProductImage> Images { get; private set; }

        internal static Product Random(Random rnd, long id)
        {
            var ret = new Product();
            ret.Id = id;
            ret.Name = "Product " + rnd.Next(100, 10000000);
            ret.Description = "Desc";
            ret.Images = new List<ProductImage>();

            int count = rnd.Next(1, 5);
            for (int i = 0; i < count; i++)
                ret.Images.Add(new ProductImage { ProductId = id, Idx = ret.Images.Count, Url = "image_" + rnd.Next(1, 10000) });

            return ret;

        }
    }
}
