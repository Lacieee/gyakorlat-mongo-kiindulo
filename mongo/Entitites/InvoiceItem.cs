namespace BME.DataDriven.Mongo.Entitites
{
    public class InvoiceItem
    {
        public string Name { get; set; }
        public int? Amount { get; set; }
        public double? Price { get; set; }
        public int? VATPercentage { get; set; }
    }
}
