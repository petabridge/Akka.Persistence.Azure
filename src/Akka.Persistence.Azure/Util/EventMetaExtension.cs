using Akka.Persistence.Azure.TableEntities;
using System.Collections.Generic;
using System.Linq;

namespace Akka.Persistence.Azure.Util
{
    internal static class EventMetaExtension
    {
        /// <summary>
        /// NOT IN USE CURRENTLY
        /// </summary>
        public static IEnumerable<EventMeta> ToRowKeys(this List<EventMetaEntry> eventMetaEntries)
        {
            var grouped = eventMetaEntries.GroupBy(x => x.PersistenceId).ToList();
            foreach(var g in grouped)
            {                
                yield return new EventMeta
                {
                    PersistenceId = g.First().PersistenceId,
                    FromSeqNo = g.Min(x=> x.SeqNo),
                    ToSeqNo = g.Max(x => x.SeqNo)
                };
            }
        }
        public static IEnumerable<EventMeta> ToEventMeta(this List<EventMetaEntry> eventMetaEntries)
        {
            var evnt = eventMetaEntries;
            foreach (var e in evnt)
            {
                yield return new EventMeta
                {
                    RowKey = long.Parse(e.RowKey),
                    PersistenceId = e.PersistenceId,
                    FromSeqNo = e.SeqNo,
                    ToSeqNo = e .SeqNo
                };
            }
        }
    }

    public class EventMeta
    {
        public long RowKey { get; set; }
        public string PersistenceId { get; set; }
        public long FromSeqNo { get; set; }
        public long ToSeqNo { get; set; }
    }
}
