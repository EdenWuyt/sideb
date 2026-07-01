import { inject, Injectable, signal } from '@angular/core';
import { environment } from '../../../environments/environment.development';
import { HttpClient } from '@angular/common/http';
import { Order as OrderType, OrderToCreate } from '../../shared/models/order';

@Injectable({
  providedIn: 'root',
})
export class Order {
  baseUrl = environment.apiUrl;
  private http = inject(HttpClient);
  orderCompleted = signal<boolean>(false);

  createOrder(orderToCreate: OrderToCreate) {
    return this.http.post<OrderType>(this.baseUrl + 'orders', orderToCreate);
  }

  getOrdersForUser() {
    return this.http.get<OrderType[]>(this.baseUrl + 'orders');
  }

  getOrderDetails(id: number) {
    return this.http.get<OrderType>(this.baseUrl + 'orders/' + id);
  }
}