using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace BME.DataDriven.Mongo.Entitites
{
    public class Category
    {
        [BsonId]
        public ObjectId ID { get; set; }
        public string Name { get; set; }
        public ObjectId? ParentCategoryID { get; set; }
    }
}