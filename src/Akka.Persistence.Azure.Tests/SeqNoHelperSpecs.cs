using System.Linq;
using Akka.Persistence.Azure.Util;
using Xunit;

namespace Akka.Persistence.Azure.Tests
{
    public class SeqNoHelperSpecs
    {
        [Fact]
        public void ShouldSortSeqnosInLexicographicalOrder()
        {
            var seqNos = new long[] {0, 1, 2, 3, 4};
            var seqNosAsRowKeys = seqNos.Select(x => x.ToJournalRowKey()).OrderByDescending(x => x).ToList();

            using (var enumerator = seqNosAsRowKeys.GetEnumerator())
            {

                foreach (var current in seqNos.OrderByDescending(y => y))
                {
                    enumerator.MoveNext();
                    Assert.Equal(enumerator.Current, current.ToJournalRowKey());
                }
            }
        }
    }
}
