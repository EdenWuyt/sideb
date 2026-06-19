using System.Linq.Expressions;

namespace Core.Interfaces;

public interface ISpecification<T>
{
    Expression<Func<T, bool>>? Criteria { get; } // where clause
    Expression<Func<T, object>>? OrderBy { get; } // for ordering
    Expression<Func<T, object>>? OrderByDescending { get; } // for ordering
    bool IsDistinct { get; } // for distinct results
}

public interface ISpecification<T, TResult> : ISpecification<T>
{
    Expression<Func<T, TResult>>? Select { get; } // for projection 
}