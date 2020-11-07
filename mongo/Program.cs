using System;
using System.Collections.Generic;
using System.Linq;
using BME.DataDriven.Mongo.Entitites;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Driver;

namespace BME.DataDriven.Mongo
{
    public static class Program
    {
        private static IMongoClient client;
        private static IMongoDatabase database;

        private static IMongoCollection<Product> productsCollection;
        private static IMongoCollection<Order> ordersCollection;
        private static IMongoCollection<Category> categoriesCollection;

        public static void Main(string[] args)
        {
            initialize();

            // TODO

            Console.ReadKey();
        }

        private static void initialize()
        {
            var pack = new ConventionPack
            {
                new ElementNameConvention(),
            };
            ConventionRegistry.Register("MyConventions", pack, _ => true);

            client = new MongoClient("mongodb://localhost:27017/datadriven");
            database = client.GetDatabase("datadriven");

            productsCollection = database.GetCollection<Product>("products");
            ordersCollection = database.GetCollection<Order>("orders");
            categoriesCollection = database.GetCollection<Category>("categories");

            Console.WriteLine("***** Első feladat *****");

            //2.1 első megoldás
            Console.WriteLine("\t2.1 1. megoldás:");
            var qProductAndStock1 = productsCollection
                .Find(p => p.Stock > 30)
                .ToList();

            foreach (var p in qProductAndStock1)
                Console.WriteLine($"\t\tName={p.Name}\tStock={p.Stock}");

            // 2.1 második megoldás
            Console.WriteLine("\t2.1 2. megoldás:");
            var qProductAndStock2 = productsCollection
                .Find(Builders<Product>.Filter.Gt(p => p.Stock, 30))
                .ToList();

            foreach (var p in qProductAndStock2)
                Console.WriteLine($"\t\tName={p.Name}\tStock={p.Stock}");

            // 2.2 első megoldás
            Console.WriteLine("\t2.2 1. megoldás:");
            var qOrderItems1 = ordersCollection
                .Find(o => o.OrderItems.Length >= 2)
                .ToList();

            foreach (var o in qOrderItems1)
                Console.WriteLine($"\t\tCustomerID={o.CustomerID}\tOrderID={o.ID}\tItems={o.OrderItems.Length}");

            //2.2 második megoldás
            Console.WriteLine("\t2.2 2. megoldás:");
            var qOrderItems2 = ordersCollection
                .Find(Builders<Order>.Filter.SizeGte(o => o.OrderItems, 2))
                .ToList();

            foreach (var o in qOrderItems2)
                Console.WriteLine($"\t\tCustomerID={o.CustomerID}\tOrderID={o.ID}\tItems={o.OrderItems.Length}");

            //2.3
            Console.WriteLine("\t2.3:");
            var qOrderTotal = ordersCollection
                .Aggregate()
                .Project(order => new
                {
                    CustomerID = order.CustomerID,
                    OrderItems = order.OrderItems,
                    Total = order.OrderItems.Sum(oi => oi.Amount * oi.Price)
                })
                .Match(order => order.Total > 30000)
                .ToList();

            foreach (var o in qOrderTotal)
            {
                Console.WriteLine($"\t\tCustomerID={o.CustomerID}");
                foreach (var oi in o.OrderItems)
                    Console.WriteLine($"\t\t\tProductID={oi.ProductID}\tPrice={oi.Price}\tAmount={oi.Amount}");
            }

            //2.4
            Console.WriteLine("\t2.4:");
            var maxPrice = productsCollection
                .Find(_ => true)
                .SortByDescending(p => p.Price)
                .Limit(1)
                .Project(p => p.Price)
                .Single();

            var qProductMax = productsCollection
                .Find(p => p.Price == maxPrice)
                .ToList();

            foreach (var t in qProductMax)
                Console.WriteLine($"\t\tName={t.Name}\tPrice={t.Price}");

            //2.5
            Console.WriteLine("\t2.5:");
            var qOrders = ordersCollection
                .Find(_ => true)
                .ToList();

            var productOrders = qOrders
                .SelectMany(o => o.OrderItems) // Egyetlen listába gyűjti a tételeket
                .GroupBy(oi => oi.ProductID)
                .Where(p => p.Count() >= 2);

            var qProducts = productsCollection
                .Find(_ => true)
                .ToList();
            var productLookup = qProducts.ToDictionary(p => p.ID);

            foreach (var p in productOrders)
            {
                var product = productLookup.GetValueOrDefault(p.Key);
                Console.WriteLine($"\t\tName={product?.Name}\tStock={product?.Stock}\tOrders={p.Count()}");
            }

            Console.WriteLine("***** Harmadik feladat *****");

            //3.1
            Console.WriteLine("\t3.1:");
            var categoryLegoId = categoriesCollection
                .Find(c => c.Name == "LEGO")
                .Project(c => c.ID)
                .Single();

            var qProductLego = productsCollection
                .Find(p => p.CategoryID == categoryLegoId)
                .ToList();
            Console.WriteLine("\t\tMódosítás előtt:");
            foreach (var p in qProductLego)
                Console.WriteLine($"\t\t\tName={p.Name}\tStock={p.Stock}\tÁr={p.Price}");

            productsCollection.UpdateMany(
                filter: p => p.CategoryID == categoryLegoId,
                update: Builders<Product>.Update.Mul(p => p.Price, 1.1));

            qProductLego = productsCollection
                .Find(p => p.CategoryID == categoryLegoId)
                .ToList();
            Console.WriteLine("\t\tMódosítás után:");
            foreach (var p in qProductLego)
                Console.WriteLine($"\t\t\tName={p.Name}\tStock={p.Stock}\tÁr={p.Price}");

            //3.2
            Console.WriteLine("\t3.2:");
            var catExpensiveToys = categoriesCollection.FindOneAndUpdate<Category>(
                filter: c => c.Name == "Expensive toys",
                update: Builders<Category>.Update.SetOnInsert(c => c.Name, "Expensive toys"),
                options: new FindOneAndUpdateOptions<Category, Category> { IsUpsert = true, ReturnDocument = ReturnDocument.After });

            productsCollection.UpdateMany(
                filter: p => p.Price > 8000,
                update: Builders<Product>.Update.Set(p => p.CategoryID, catExpensiveToys.ID));

            var qProdExpensive = productsCollection
                .Find(p => p.CategoryID == catExpensiveToys.ID)
                .ToList();
            foreach (var p in qProdExpensive)
                Console.WriteLine($"\t\tName={p.Name}\tPrice={p.Price}");

            //3.3
            Console.WriteLine("\t3.3:");
            Console.WriteLine($"\t\tMódosítás előtt {categoriesCollection.CountDocuments(_ => true)} db kategória");

            var qProductCategory = new HashSet<ObjectId>(
                productsCollection
                    .Find(_ => true)
                    .Project(p => p.CategoryID)
                    .ToList());

            categoriesCollection.DeleteMany(c => !qProductCategory.Contains(c.ID));

            Console.WriteLine($"\t\tMódosítás után {categoriesCollection.CountDocuments(_ => true)} db kategória");

        }
    }
}
