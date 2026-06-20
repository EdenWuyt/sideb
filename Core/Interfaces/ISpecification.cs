using System.Linq.Expressions;

namespace Core.Interfaces;

public interface ISpecification<T>
{
    Expression<Func<T, bool>>? Criteria { get; } // where clause
    Expression<Func<T, object>>? OrderBy { get; } // for ordering
    Expression<Func<T, object>>? OrderByDescending { get; } // for ordering
    bool IsDistinct { get; } // for distinct results
    int Take { get; } // for pagination
    int Skip { get; } // for pagination
    bool IsPagingEnabled { get; } // for pagination
    IQueryable<T> ApplyCriteria(IQueryable<T> query); // to apply the specification to a queryable
}

public interface ISpecification<T, TResult> : ISpecification<T>
{
    Expression<Func<T, TResult>>? Select { get; } // for projection 
}