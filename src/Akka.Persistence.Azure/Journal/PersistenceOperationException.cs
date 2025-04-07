using System;

namespace Akka.Persistence.Azure.Journal;

public class PersistenceOperationException: Exception
{
    public PersistenceOperationException(string message) : base(message)
    {
    }

    public PersistenceOperationException(string message, Exception innerException) : base(message, innerException)
    {
    }
}