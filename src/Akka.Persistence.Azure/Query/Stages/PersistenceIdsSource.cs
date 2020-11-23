//-----------------------------------------------------------------------
// <copyright file="PersistenceIdsSource.cs" company="Akka.NET Project">
//     Copyright (C) 2017 Akka.NET Contrib <https://github.com/AkkaNetContrib/Akka.Persistence.Redis>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Streams.Stage;
using System.Collections.Generic;
using Akka.Actor;
using Akka.Streams;
using Microsoft.Azure.Cosmos.Table;
using Akka.Persistence.Azure.TableEntities;
using System.Threading.Tasks;
using System.Collections.Immutable;
using System.Linq;
using System.Reactive.Linq;
using Nito.AsyncEx;

namespace Akka.Persistence.Azure.Query.Stages
{
    internal class PersistenceIdsSource : GraphStage<SourceShape<string>>
    {
        private readonly CloudTable _cloudTable;
        private readonly ExtendedActorSystem _system;

        public PersistenceIdsSource(CloudTable cloudTable, ExtendedActorSystem system)
        {
            _cloudTable = cloudTable;
            _system = system;            
        }

        public Outlet<string> Outlet { get; } = new Outlet<string>(nameof(PersistenceIdsSource));

        public override SourceShape<string> Shape => new SourceShape<string>(Outlet);

        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
        {
            return new PersistenceIdsLogic(_cloudTable, _system, Outlet, Shape);
        }

        private class PersistenceIdsLogic : GraphStageLogic
        {
            private bool _start = true;
            private DateTimeOffset? _index = null;
            private CloudTable _cloudTable;
            private readonly Queue<string> _buffer = new Queue<string>();
            private bool _downstreamWaiting = false;
            private IObservable<IEnumerable<AllPersistenceIdsEntry>> _observer;

            private readonly Outlet<string> _outlet;
           

            public PersistenceIdsLogic(CloudTable cloudTable, ExtendedActorSystem system, Outlet<string> outlet, Shape shape) : base(shape)
            {
                _cloudTable = cloudTable;
                _outlet = outlet;
                SetHandler(outlet, onPull: () =>
                {
                    _downstreamWaiting = true;
                    if (_buffer.Count == 0 && (_start || _index.HasValue))
                    {
                        var callback = GetAsyncCallback<IEnumerable<AllPersistenceIdsEntry>>(data =>
                        {
                            // save the index for further initialization if needed
                            //Descending sort, so the first one will the highest Timestamp
                            _index = data.FirstOrDefault().Timestamp;

                            // it is not the start anymore
                            _start = false;

                            // enqueue received data
                            try
                            {
                                foreach (var item in data)
                                {
                                    _buffer.Enqueue(item.RowKey);
                                }
                            }
                            catch (Exception e)
                            {
                                Log.Error(e, "Error while querying persistence identifiers");
                                FailStage(e);
                            }

                            // deliver element
                            Deliver();
                        });
                        //This may block once in a while
                        //var res = GetAllPersistenceIds(_index).Result;
                        //callback(res);
                        callback(SynchronizationContextSwitcher.NoContext(async () => await GetAllPersistenceIds(_index)).Result);
                    }
                    else if (_buffer.Count == 0)
                    {
                        // wait for asynchornous notification and mark dowstream
                        // as waiting for data
                    }
                    else
                    {
                        Deliver();
                    }
                });
            }

            public override void PreStart()
            {
                var callback = GetAsyncCallback<IEnumerable<AllPersistenceIdsEntry>>(data =>
                {
                    Log.Debug("Message received");

                    // enqueue the element
                    foreach(var d in data)
                        _buffer.Enqueue(d.RowKey);

                    // deliver element
                    Deliver();
                });

                _observer = Observable.FromAsync(c => GetAllPersistenceIds(_index));
                _observer.Subscribe(x=> callback.Invoke(x));
            }

            public override void PostStop()
            {
                //trying to dispose
                _observer = null;
            }

            private void Deliver()
            {
                if (_downstreamWaiting)
                {
                    _downstreamWaiting = false;
                    var elem = _buffer.Dequeue();
                    Push(_outlet, elem);
                }
            }

            private async Task<IEnumerable<AllPersistenceIdsEntry>> GetAllPersistenceIds(DateTimeOffset? offset)
            {
                TableQuerySegment<AllPersistenceIdsEntry> result = null;

                var returnValue = ImmutableList<AllPersistenceIdsEntry>.Empty;

                do
                {
                    result = await _cloudTable.ExecuteQuerySegmentedAsync(PrepareQuery(offset), result?.ContinuationToken);

                    if (result.Results.Count > 0)
                    {
                        returnValue = returnValue.AddRange(result.Results.OrderByDescending(x=> x.Timestamp));
                    }
                } while (result.ContinuationToken != null);

                return returnValue;
            }
            private TableQuery<AllPersistenceIdsEntry> PrepareQuery(DateTimeOffset? offset = null)
            {
                var filter = TableQuery.GenerateFilterCondition(
                             "PartitionKey",
                             QueryComparisons.Equal,
                             AllPersistenceIdsEntry.PartitionKeyValue);

                //for a live query, we need to start from a log time greater than the previous query
                //https://stackoverflow.com/questions/56023365/how-to-retrieve-data-from-azure-table-storage-between-date-and-time
                if (offset != null)
                {
                    filter = TableQuery.CombineFilters(filter, TableOperators.And,
                        TableQuery.GenerateFilterCondition(
                             "logdatetime",
                             QueryComparisons.GreaterThan,
                             offset.Value.ToString())
                        );
                }
                return new TableQuery<AllPersistenceIdsEntry>().Where(filter); 
            }
        }
    }
}
