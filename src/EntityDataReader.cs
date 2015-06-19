using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace ImportTest
{
    internal class EnumerableDataReader<TSource> : IDataReader
    {
        private IList<Func<TSource, object>> columns = new List<Func<TSource, object>>();
        private IEnumerator<TSource> source;

        internal EnumerableDataReader(IEnumerable<TSource> source)
        {
            this.source = source.GetEnumerator();
        }

        public EnumerableDataReader<TSource> AddColumn(Func<TSource,object> columnGetter)
        {
            this.columns.Add(columnGetter);
            return this;
        }

        public void Dispose()
        {
            this.source.Dispose();
            this.source = null;
        }

        public bool IsDBNull(int i)
        {
            return GetValue(i) == null;
        }

        public object this[string name]
        {
            get { return GetValue(GetOrdinal(name)); }
        }

        public object this[int i]
        {
            get { return GetValue(i); }
        }

        public object GetValue(int i)
        {
            return this.columns[i](this.source.Current);
        }

        public int FieldCount
        {
            get { return this.columns.Count; }
        }

        public bool Read()
        {
            return this.source.MoveNext();
        }

        public bool GetBoolean(int i) { return (bool)GetValue(i); }
        public byte GetByte(int i) { return (byte)GetValue(i); }
        public char GetChar(int i) { return (char)GetValue(i); }
        public DateTime GetDateTime(int i) { return (DateTime)GetValue(i); }
        public decimal GetDecimal(int i) { return (decimal)GetValue(i); }
        public double GetDouble(int i) { return (double)GetValue(i); }
        public float GetFloat(int i) { return (float)GetValue(i); }
        public Guid GetGuid(int i) { return (Guid)GetValue(i); }
        public short GetInt16(int i) { return (short)GetValue(i); }
        public int GetInt32(int i) { return (int)GetValue(i); }
        public long GetInt64(int i) { return (long)GetValue(i); }
        public string GetString(int i) { return (string)GetValue(i); }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) { throw new NotSupportedException(); }
        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) { throw new NotSupportedException(); }


        public IDataReader GetData(int i) { throw new NotSupportedException(); }
        public Type GetFieldType(int i) { throw new NotSupportedException(); }
        public string GetDataTypeName(int i) { throw new NotSupportedException(); }
        public string GetName(int i) { throw new NotSupportedException(); }
        public int GetOrdinal(string name) { throw new NotSupportedException(); }
        public DataTable GetSchemaTable() { throw new NotSupportedException(); }
        public bool NextResult() { throw new NotSupportedException(); }
        public int RecordsAffected { get { throw new NotSupportedException(); } }
        public int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public void Close()
        {
            this.Dispose();
        }

        public int Depth
        {
            get { return 1; }
        }

        public bool IsClosed
        {
            get { return this.source == null; }
        }

       
    }
}
