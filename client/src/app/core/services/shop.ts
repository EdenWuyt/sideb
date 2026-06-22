import { HttpClient, HttpParams } from '@angular/common/http';
import { inject, Injectable } from '@angular/core';
import { Pagination } from '../../shared/models/pagination';
import { Product } from '../../shared/models/product';
import { ShopParams } from '../../shared/models/shopParams';

@Injectable({
  providedIn: 'root',
})
export class Shop {
  baseUrl = "https://localhost:5001/api/";
  private http = inject(HttpClient);
  genres: string[] = [];
  
  getProducts(shopParams: ShopParams) {
    let params = new HttpParams();
    
    if (shopParams.genres && shopParams.genres.length > 0) {
      params = params.append("genres", shopParams.genres.join(","));
    }
    
    if (shopParams.sort) {
      params = params.append("sort", shopParams.sort);
    }
    
    if (shopParams.search) {
      params = params.append("search", shopParams.search);
    }

    params = params.append("pageSize", shopParams.pageSize.toString());
    params = params.append("pageIndex", shopParams.pageIndex.toString());
    return this.http.get<Pagination<Product>>(this.baseUrl + "products", { params });
  }

  getProduct(id: number) {
    return this.http.get<Product>(this.baseUrl + "products/" + id);
  }

  getGenres() {
    if (this.genres.length > 0) return;
    return this.http.get<string[]>(this.baseUrl + "products/genres").subscribe({
      next: (response) => this.genres = response,
      error: (error) => console.error(error)
    });
  }
}
