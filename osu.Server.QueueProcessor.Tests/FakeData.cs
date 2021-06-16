using System;

namespace osu.Server.QueueProcessor.Tests
{
    public class FakeData : QueueItem, IEquatable<FakeData>
    {
        public readonly Guid Data;

        public FakeData(Guid data)
        {
            Data = data;
        }

        public static FakeData New() => new FakeData(Guid.NewGuid());

        public override string ToString() => Data.ToString();

        public bool Equals(FakeData? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;

            return Data.Equals(other.Data);
        }

        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;

            return Equals((FakeData)obj);
        }

        public override int GetHashCode()
        {
            return Data.GetHashCode();
        }
    }
}
