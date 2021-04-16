using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace Akka.Persistence.Azure
{
    public static class ListExtensions
    {
        public static IImmutableList<IEnumerable<T>> ChunkBy<T>(this IEnumerable<T> source, int chunkSize)
        {
            return source
                .Select((x, i) => new { Index = i, Value = x })
                .GroupBy(x => x.Index / chunkSize)
                .Select(x => x.Select(v => v.Value).ToList().AsEnumerable())
                .ToImmutableList();
        }
    }
}