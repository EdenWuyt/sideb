import { Component, signal, inject, OnInit } from '@angular/core';
import { Header } from "./layout/header/header";
import { HttpClient } from '@angular/common/http';
import { type Product } from './shared/models/product';
import { type Pagination } from './shared/models/pagination';

@Component({
  selector: 'app-root',
  imports: [Header],
  templateUrl: './app.html',
  styleUrl: './app.css'
})
export class App implements OnInit {
  baseUrl = "https://localhost:5001/api/";
  private http = inject(HttpClient);

  protected readonly title = signal('SideB Record');
  protected readonly products = signal<Product[]>([]);

  ngOnInit(): void {
    this.http.get<Pagination<Product>>(this.baseUrl + "products").subscribe({
      next: (response) => {
        this.products.set(response.data);
        console.log(this.products());
      },
      error: (error) => {
        console.error(error);
      },
      complete: () => {console.log("Request completed");
      }
    })
  }
}
